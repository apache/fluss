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
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests value decoders against the shared fixed-column table contract. */
class KafkaFormatFactoryRegistryTest {
    private final KafkaFormatFactoryRegistry formats = new KafkaFormatFactoryRegistry();

    @Test
    void testBinaryDecoderPreservesOpaqueBytesAndNull() {
        KafkaFieldDecoder decoder = formats.createDecoder(schema(false));
        byte[] opaque = new byte[] {(byte) 0xff, 0};
        assertThat((byte[]) decoder.decode(ByteBuffer.wrap(opaque)).valueFields()[0])
                .containsExactly((byte) 0xff, (byte) 0);
        assertThat(decoder.decode(null).valueFields()).containsExactly((Object) null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"heap", "direct", "read-only"})
    void testStringDecoderPreservesBufferRangeAndBorrowing(String kind) {
        KafkaFieldDecoder decoder = formats.createDecoder(schema(true));
        byte[] expected = "消息-😀".getBytes(StandardCharsets.UTF_8);
        ByteBuffer storage =
                kind.equals("direct")
                        ? ByteBuffer.allocateDirect(expected.length + 4)
                        : ByteBuffer.allocate(expected.length + 4);
        storage.position(2);
        storage.put(expected);
        storage.position(1);
        ByteBuffer slice = storage.slice();
        slice.position(1);
        slice.limit(1 + expected.length);
        ByteBuffer input = kind.equals("read-only") ? slice.asReadOnlyBuffer() : slice;
        BinaryString decoded = (BinaryString) decoder.decode(input).valueFields()[0];
        assertThat(decoded.toBytes()).isEqualTo(expected);
        assertThat(input.position()).isEqualTo(1);
        assertThat(input.remaining()).isEqualTo(expected.length);
        // Direct and array-backed buffers are borrowed; read-only heap buffers need a copy.
        storage.put(2, (byte) 'x');
        if (!kind.equals("read-only")) {
            expected[0] = 'x';
        }
        assertThat(decoded.toBytes()).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFactoriesUseThePublicValueProjection(boolean string) {
        KafkaTableSchema schema = schema(string);
        assertThat(schema.valueProjection().positions()).containsExactly(1);
        KafkaFieldDecoder decoder = formats.createDecoder(schema);
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        KafkaDecodedValue decoded = decoder.decode(ByteBuffer.wrap(payload));
        assertThat(decoded.valueFields())
                .containsExactly(string ? BinaryString.fromString("payload") : payload);
        assertThat(decoded.rescue()).isNull();
    }

    private static KafkaTableSchema schema(boolean string) {
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
        return KafkaTableSchema.resolve(table);
    }
}
