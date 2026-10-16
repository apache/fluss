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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TimestampType;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Verifies fixed-column Arrow records after transcoding resources have closed. */
class ArrowKafkaRecordTranscoderTest {
    private final ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFixedColumnsAndAllOptionalSystemColumnCombinations(boolean string) throws Exception {
        // The value looks like JSON but binary/string must not interpret or rescue its members.
        byte[] value = bytes("{\"_kafka_rescue\":\"消息\",\"_kafka_key\":123}");
        byte[] key = {(byte) 0xff, 0, (byte) 0xc3};
        Record record =
                new Record(
                        123L,
                        TimestampType.LOG_APPEND_TIME,
                        key,
                        value,
                        Arrays.asList(
                                new RecordHeader("duplicate", bytes("first")),
                                new RecordHeader("nullable", bytes("discarded")),
                                new RecordHeader("duplicate", bytes("last")),
                                new RecordHeader("nullable", null)));
        for (int mask = 0; mask < 16; mask++) {
            TableInfo table = envelope(string, mask);
            TableDescriptor before = table.toTableDescriptor();
            KafkaTableSchema contract = KafkaTableSchema.resolve(before);
            read(
                    transcoder.transcode(7, Collections.singletonList(record), table),
                    table,
                    1,
                    row -> {
                        assertThat(row.getFieldCount())
                                .isEqualTo(table.getRowType().getFieldCount());
                        assertValue(row, contract.valueProjection().positionAt(0), string, value);
                        if (contract.keyPosition() >= 0) {
                            assertThat(row.getBytes(contract.keyPosition())).isEqualTo(key);
                        }
                        if (contract.headersPosition() >= 0) {
                            Map<String, byte[]> headers =
                                    headers(row.getMap(contract.headersPosition()));
                            assertThat(headers).hasSize(2).containsKey("nullable");
                            assertThat(headers.get("duplicate")).isEqualTo(bytes("last"));
                            assertThat(headers.get("nullable")).isNull();
                        }
                        if (contract.metadataPosition() >= 0) {
                            assertMetadata(row, contract.metadataPosition(), 123L, 1, 7);
                        }
                        if (contract.rescuePosition() >= 0) {
                            assertThat(row.isNullAt(contract.rescuePosition())).isTrue();
                        }
                    });
            assertThat(table.toTableDescriptor()).isEqualTo(before);
            assertThat(before.getProperties()).doesNotContainKey("table.kafka.convert.key.type");
            assertThat(before.getCustomProperties()).isEmpty();
        }
        assertThat(record.timestampType()).isEqualTo(TimestampType.LOG_APPEND_TIME);
        assertThat(record.timestamp()).isEqualTo(123L);
        assertThat(record.borrowedKey()).isEqualTo(key);
        assertThat(record.borrowedValue()).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNullAndEmptyPayloadsRemainDistinct(boolean string) throws Exception {
        TableInfo table = envelope(string, 7);
        List<Record> records =
                Arrays.asList(
                        new Record(Record.NO_TIMESTAMP, null, null, Collections.emptyList()),
                        new Record(0L, new byte[0], new byte[0], Collections.emptyList()));
        AtomicInteger next = new AtomicInteger();
        read(
                transcoder.transcode(0, records, table),
                table,
                2,
                row -> {
                    int value = table.getRowType().getFieldIndex("_kafka_value");
                    int key = table.getRowType().getFieldIndex("_kafka_key");
                    if (next.getAndIncrement() == 0) {
                        assertThat(row.isNullAt(value)).isTrue();
                        assertThat(row.isNullAt(key)).isTrue();
                    } else {
                        assertValue(row, value, string, new byte[0]);
                        assertThat(row.getBytes(key)).isEmpty();
                    }
                    assertThat(
                                    row.getMap(table.getRowType().getFieldIndex("_kafka_headers"))
                                            .size())
                            .isZero();
                    assertThat(row.isNullAt(table.getRowType().getFieldIndex("_kafka_rescue")))
                            .isTrue();
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testKeyNullabilityAndRawKeyBytes(boolean string) throws Exception {
        TableInfo table =
                table(
                        descriptor(
                                        Schema.newBuilder()
                                                .column("_kafka_key", DataTypes.BYTES().copy(false))
                                                .column("_kafka_value", valueType(string))
                                                .build(),
                                        string)
                                .build());
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        0,
                                        Collections.singletonList(
                                                new Record(
                                                        1L,
                                                        null,
                                                        bytes("valid"),
                                                        Collections.emptyList())),
                                        table))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("NOT NULL _kafka_key");
        byte[] invalidUtf8Key = {(byte) 0xff};
        read(
                transcoder.transcode(
                        0,
                        Collections.singletonList(
                                new Record(
                                        1L,
                                        invalidUtf8Key,
                                        bytes("valid"),
                                        Collections.emptyList())),
                        table),
                table,
                1,
                row -> assertThat(row.getBytes(0)).isEqualTo(invalidUtf8Key));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testHeaderNullabilityIsCheckedAfterLastValueWins(boolean nullableValue) throws Exception {
        TableInfo table =
                table(
                        descriptor(
                                        Schema.newBuilder()
                                                .column(
                                                        "_kafka_headers",
                                                        DataTypes.MAP(
                                                                        DataTypes.STRING()
                                                                                .copy(false),
                                                                        DataTypes.BYTES()
                                                                                .copy(
                                                                                        nullableValue))
                                                                .copy(false))
                                                .column("_kafka_value", DataTypes.STRING())
                                                .build(),
                                        true)
                                .build());
        List<RecordHeader> overwrittenNull =
                Arrays.asList(
                        new RecordHeader("same", null),
                        new RecordHeader("", new byte[0]),
                        new RecordHeader("same", bytes("last")));
        read(
                transcoder.transcode(
                        0,
                        Collections.singletonList(
                                new Record(1L, null, bytes("v"), overwrittenNull)),
                        table),
                table,
                1,
                row -> {
                    Map<String, byte[]> map = headers(row.getMap(0));
                    assertThat(map).hasSize(2);
                    assertThat(map.get("same")).isEqualTo(bytes("last"));
                    assertThat(map.get("")).isEmpty();
                });
        Record finalNull =
                new Record(
                        1L,
                        null,
                        bytes("v"),
                        Arrays.asList(
                                new RecordHeader("same", bytes("first")),
                                new RecordHeader("same", null)));
        if (nullableValue) {
            read(
                    transcoder.transcode(0, Collections.singletonList(finalNull), table),
                    table,
                    1,
                    row ->
                            assertThat(headers(row.getMap(0)))
                                    .containsOnlyKeys("same")
                                    .containsEntry("same", null));
        } else {
            assertThatThrownBy(
                            () ->
                                    transcoder.transcode(
                                            0, Collections.singletonList(finalNull), table))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("NOT NULL _kafka_headers map values");
        }
        read(
                transcoder.transcode(
                        0,
                        Collections.singletonList(
                                new Record(1L, null, bytes("v"), Collections.emptyList())),
                        table),
                table,
                1,
                row -> assertThat(row.getMap(0).size()).isZero());
    }

    @Test
    void testOmittedSystemColumnsIgnoreTimestampKeyAndHeaders() throws Exception {
        TableInfo table = valueTable(false);
        List<Record> records = new ArrayList<>();
        for (TimestampType type : TimestampType.values()) {
            for (long timestamp : new long[] {Record.NO_TIMESTAMP, 0L, Long.MAX_VALUE}) {
                records.add(
                        new Record(
                                timestamp,
                                type,
                                new byte[] {(byte) 0xff},
                                bytes("v"),
                                Collections.singletonList(new RecordHeader("ignored", null))));
            }
        }
        read(
                transcoder.transcode(0, records, table),
                table,
                records.size(),
                row -> {
                    assertThat(row.getFieldCount()).isEqualTo(1);
                    assertThat(row.getString(0).toString()).isEqualTo("v");
                });
    }

    @Test
    void testBinaryValuePreservesArbitraryBytesAcrossOutputPages() throws Exception {
        byte[] value = new byte[32768];
        new Random(42L).nextBytes(value);
        value[0] = (byte) 0xff;
        TableInfo table = envelope(false, 4);
        BytesView encoded =
                transcoder.transcode(
                        0,
                        Collections.singletonList(
                                new Record(1L, null, value, Collections.emptyList())),
                        table);
        assertThat(encoded.getBytesLength()).isGreaterThan(4096);
        read(
                encoded,
                table,
                1,
                row -> {
                    assertThat(row.isNullAt(0)).isTrue();
                    assertThat(row.getBytes(1)).isEqualTo(value);
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testValueNotNullConstraint(boolean string) {
        TableInfo table =
                table(
                        descriptor(
                                        Schema.newBuilder()
                                                .column(
                                                        "_kafka_value",
                                                        valueType(string).copy(false))
                                                .build(),
                                        string)
                                .build());
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        0,
                                        Collections.singletonList(
                                                new Record(
                                                        1L, null, null, Collections.emptyList())),
                                        table))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("NOT NULL field '_kafka_value'");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMetadataPreservesInputTimestampBatchTypeAndRequestedPartition(boolean string)
            throws Exception {
        long[] timestamps = {Record.NO_TIMESTAMP, 0L, 123L, 1728912345678L, Long.MAX_VALUE};
        TimestampType[] types = {
            TimestampType.UNKNOWN, TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME
        };
        int[] typeIds = {-1, 0, 1};
        List<Record> records = new ArrayList<>();
        for (TimestampType type : types) {
            for (long timestamp : timestamps) {
                records.add(new Record(timestamp, type, null, bytes("v"), Collections.emptyList()));
            }
        }
        for (boolean nullable : new boolean[] {false, true}) {
            TableInfo table = metadataTable(string, nullable, true);
            TableDescriptor before = table.toTableDescriptor();
            // Reuse the same converter and records across requests without retaining partition
            // state.
            for (int partition : new int[] {7, 0, 3}) {
                AtomicInteger next = new AtomicInteger();
                read(
                        transcoder.transcode(partition, records, table),
                        table,
                        records.size(),
                        row -> {
                            int i = next.getAndIncrement();
                            assertMetadata(
                                    row,
                                    0,
                                    timestamps[i % timestamps.length],
                                    typeIds[i / timestamps.length],
                                    partition);
                            assertValue(row, 1, string, bytes("v"));
                        });
            }
            assertThat(table.toTableDescriptor()).isEqualTo(before);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMetadataTimestampNotNullFailureHasNoPartialOutputAndRemainsUsable(boolean string)
            throws Exception {
        Record valid =
                new Record(
                        0L, TimestampType.CREATE_TIME, null, bytes("v"), Collections.emptyList());
        for (boolean nullable : new boolean[] {false, true}) {
            TableInfo table = metadataTable(string, nullable, false);
            for (TimestampType type : TimestampType.values()) {
                Record invalid =
                        new Record(
                                Record.NO_TIMESTAMP,
                                type,
                                null,
                                bytes("v"),
                                Collections.emptyList());
                AtomicReference<BytesView> output = new AtomicReference<>();
                Throwable failure =
                        catchThrowable(
                                () ->
                                        output.set(
                                                transcoder.transcode(
                                                        7, Arrays.asList(valid, invalid), table)));
                assertThat(failure)
                        .isInstanceOf(KafkaRecordEncodingException.class)
                        .hasMessageContaining("NOT NULL _kafka_metadata.timestamp");
                assertThat(failure.getSuppressed()).isEmpty();
                assertThat(output).hasNullValue();
                read(
                        transcoder.transcode(3, Collections.singletonList(valid), table),
                        table,
                        1,
                        row -> {
                            assertMetadata(row, 0, 0L, 0, 3);
                            assertValue(row, 1, string, bytes("v"));
                        });
            }
        }
    }

    @Test
    void testLegacyRecordDoesNotInferBatchTimestampType() throws Exception {
        TableInfo table = metadataTable(true, false, false);
        read(
                transcoder.transcode(
                        7,
                        Collections.singletonList(
                                new Record(123L, null, bytes("v"), Collections.emptyList())),
                        table),
                table,
                1,
                row -> assertMetadata(row, 0, 123L, -1, 7));
    }

    @Test
    void testRejectsNegativePartitionId() {
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        -1,
                                        Collections.singletonList(
                                                new Record(
                                                        Record.NO_TIMESTAMP,
                                                        null,
                                                        bytes("v"),
                                                        Collections.emptyList())),
                                        envelope(false, 8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Kafka partition ID must be non-negative");
    }

    @Test
    void testRejectsLegacyMetadataOffsetField() {
        Schema schema =
                Schema.newBuilder()
                        .column("_kafka_value", DataTypes.STRING())
                        .column(
                                "_kafka_metadata",
                                DataTypes.ROW(
                                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                        DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                        DataTypes.FIELD("partition", DataTypes.INT()),
                                        DataTypes.FIELD("offset", DataTypes.BIGINT())))
                        .build();
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        0,
                                        Collections.singletonList(
                                                new Record(
                                                        123L,
                                                        TimestampType.CREATE_TIME,
                                                        null,
                                                        bytes("v"),
                                                        Collections.emptyList())),
                                        table(descriptor(schema, true).build())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining(
                        "_kafka_metadata must be ROW<timestamp TIMESTAMP_LTZ(3), "
                                + "timestamp_type INT, partition INT>");
    }

    @Test
    void testRejectsDisabledAndLegacyContractsAndEmptyPartitions() {
        List<Record> records =
                Collections.singletonList(
                        new Record(1L, null, bytes("v"), Collections.emptyList()));
        Schema fixed = Schema.newBuilder().column("_kafka_value", DataTypes.STRING()).build();
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        0,
                                        records,
                                        table(
                                                descriptor(fixed, true)
                                                        .property(
                                                                ConfigOptions.TABLE_KAFKA_ENABLED,
                                                                false)
                                                        .build())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("disabled");
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        0,
                                        records,
                                        table(
                                                descriptor(
                                                                Schema.newBuilder()
                                                                        .column(
                                                                                "body",
                                                                                DataTypes.STRING())
                                                                        .build(),
                                                                true)
                                                        .build())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("_kafka_value");
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        0,
                                        records,
                                        table(
                                                descriptor(fixed, true)
                                                        .customProperty(
                                                                "kafka.value.format", "string")
                                                        .build())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("custom property");
        assertThatThrownBy(
                        () -> transcoder.transcode(0, Collections.emptyList(), valueTable(false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void testJsonIsNotActivatedEvenForNullValues() {
        TableInfo table =
                table(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("body", DataTypes.STRING())
                                                .build())
                                .distributedBy(1)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "parse_json")
                                .build());
        for (byte[] value : new byte[][] {null, bytes("{}")}) {
            assertThatThrownBy(
                            () ->
                                    transcoder.transcode(
                                            0,
                                            Collections.singletonList(
                                                    new Record(
                                                            1L,
                                                            null,
                                                            value,
                                                            Collections.emptyList())),
                                            table))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("PARSE_JSON");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStreamingBatchPreservesDistinctRecordsBeyondInitialCapacity(boolean string)
            throws Exception {
        TableInfo table = envelope(string, 15);
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 2050; i++) {
            records.add(
                    new Record(
                            i % 3 == 0 ? Record.NO_TIMESTAMP : i,
                            i % 3 == 0
                                    ? TimestampType.UNKNOWN
                                    : (i % 3 == 1
                                            ? TimestampType.CREATE_TIME
                                            : TimestampType.LOG_APPEND_TIME),
                            bytes("key-" + i),
                            bytes("消息-" + i),
                            Arrays.asList(
                                    new RecordHeader("duplicate", null),
                                    new RecordHeader("duplicate", bytes("header-" + i)))));
        }
        AtomicInteger next = new AtomicInteger();
        read(
                transcoder.transcode(7, records, table),
                table,
                records.size(),
                row -> {
                    int i = next.getAndIncrement();
                    assertValue(row, 2, string, bytes("消息-" + i));
                    assertThat(row.getBytes(3)).isEqualTo(bytes("key-" + i));
                    Map<String, byte[]> map = headers(row.getMap(0));
                    assertThat(map).hasSize(1);
                    assertThat(map.get("duplicate")).isEqualTo(bytes("header-" + i));
                    assertThat(row.isNullAt(1)).isTrue();
                    assertMetadata(row, 4, i % 3 == 0 ? Record.NO_TIMESTAMP : i, i % 3 - 1, 7);
                });
    }

    @Test
    void testOneRecordWithManyDistinctHeadersGrowsArrowMapVectors() throws Exception {
        TableInfo table = envelope(false, 2);
        List<RecordHeader> input = new ArrayList<>();
        for (int i = 0; i < 2050; i++) {
            input.add(new RecordHeader("header-" + i, bytes("value-" + i)));
        }
        read(
                transcoder.transcode(
                        0,
                        Collections.singletonList(new Record(1L, null, bytes("v"), input)),
                        table),
                table,
                1,
                row -> {
                    Map<String, byte[]> map = headers(row.getMap(0));
                    assertThat(map).hasSize(2050);
                    for (int i = 0; i < 2050; i++) {
                        assertThat(map.get("header-" + i)).isEqualTo(bytes("value-" + i));
                    }
                });
    }

    @Test
    void testStreamingEncoderCopiesRowAndPayloadBeforeSourceReusesThem() throws Exception {
        TableInfo table = envelope(false, 7);
        byte[] key = new byte[1];
        byte[] value = new byte[1];
        byte[] headerValue = new byte[1];
        GenericRow row =
                GenericRow.of(
                        GenericMap.of(BinaryString.fromString("header"), headerValue),
                        null,
                        value,
                        key);
        BytesView encoded =
                new FlussArrowRecordEncoder()
                        .encodeStreaming(
                                consumer -> {
                                    for (int i = 0; i < 3; i++) {
                                        key[0] = (byte) i;
                                        value[0] = (byte) (i + 10);
                                        headerValue[0] = (byte) (i + 20);
                                        consumer.append(row);
                                    }
                                    Arrays.fill(key, (byte) -1);
                                    Arrays.fill(value, (byte) -1);
                                    Arrays.fill(headerValue, (byte) -1);
                                    row.setField(2, null);
                                },
                                table);
        AtomicInteger next = new AtomicInteger();
        read(
                encoded,
                table,
                3,
                actual -> {
                    int i = next.getAndIncrement();
                    assertThat(actual.getBytes(3)).containsExactly((byte) i);
                    assertThat(actual.getBytes(2)).containsExactly((byte) (i + 10));
                    assertThat(headers(actual.getMap(0)).get("header"))
                            .containsExactly((byte) (i + 20));
                });
    }

    @Test
    void testDecodeFailureAfterValidRecordHasNoPartialOutputAndRemainsUsable() throws Exception {
        TableInfo table = valueTable(true);
        Record valid = new Record(1L, null, bytes("valid"), Collections.emptyList());
        for (byte[] invalid : new byte[][] {null, {(byte) 0xc3, 0x28}, {(byte) 0xff}}) {
            AtomicReference<BytesView> output = new AtomicReference<>();
            Throwable failure =
                    catchThrowable(
                            () ->
                                    output.set(
                                            transcoder.transcode(
                                                    0,
                                                    Arrays.asList(
                                                            valid,
                                                            new Record(
                                                                    2L,
                                                                    null,
                                                                    invalid,
                                                                    Collections.emptyList())),
                                                    table)));
            assertThat(failure).isInstanceOf(KafkaRecordEncodingException.class);
            assertThat(failure.getSuppressed()).isEmpty();
            assertThat(output).hasNullValue();
        }
        read(
                transcoder.transcode(0, Collections.singletonList(valid), table),
                table,
                1,
                row -> assertThat(row.getString(0).toString()).isEqualTo("valid"));
    }

    @Test
    void testHeaderFailureAfterValidRecordHasNoPartialOutputAndRemainsUsable() throws Exception {
        TableInfo table =
                table(
                        descriptor(
                                        Schema.newBuilder()
                                                .column("_kafka_value", DataTypes.STRING())
                                                .column(
                                                        "_kafka_headers",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.BYTES().copy(false)))
                                                .build(),
                                        true)
                                .build());
        Record valid =
                new Record(
                        1L,
                        null,
                        bytes("v"),
                        Collections.singletonList(new RecordHeader("h", bytes("v"))));
        Record invalid =
                new Record(
                        1L,
                        null,
                        bytes("v"),
                        Collections.singletonList(new RecordHeader("h", null)));
        AtomicReference<BytesView> output = new AtomicReference<>();
        Throwable failure =
                catchThrowable(
                        () ->
                                output.set(
                                        transcoder.transcode(
                                                0, Arrays.asList(valid, invalid), table)));
        assertThat(failure).isInstanceOf(KafkaRecordEncodingException.class);
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(output).hasNullValue();
        read(
                transcoder.transcode(0, Collections.singletonList(valid), table),
                table,
                1,
                row -> assertThat(headers(row.getMap(1)).get("h")).isEqualTo(bytes("v")));
    }

    @Test
    void testArrowWriterFailureClosesAllocatorWithoutLeaking() {
        Throwable failure =
                catchThrowable(
                        () ->
                                new FlussArrowRecordEncoder()
                                        .encode(
                                                Collections.singletonList(GenericRow.of(123)),
                                                valueTable(false)));
        assertThat(failure).isInstanceOf(ClassCastException.class);
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @Test
    void testStreamingSourceFailureClosesAllocator() {
        IOException expected = new IOException("source failed after the first row");
        Throwable failure =
                catchThrowable(
                        () ->
                                new FlussArrowRecordEncoder()
                                        .encodeStreaming(
                                                consumer -> {
                                                    consumer.append(
                                                            GenericRow.of(
                                                                    BinaryString.fromString(
                                                                            "valid")));
                                                    throw expected;
                                                },
                                                valueTable(false)));
        assertThat(failure).isSameAs(expected);
        assertThat(failure.getSuppressed()).isEmpty();
    }

    private static TableInfo valueTable(boolean notNull) {
        return table(
                descriptor(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.STRING().copy(!notNull))
                                        .build(),
                                true)
                        .build());
    }

    private static TableInfo envelope(boolean string, int mask) {
        Schema.Builder schema = Schema.newBuilder();
        if ((mask & 2) != 0) {
            schema.column("_kafka_headers", DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
        }
        if ((mask & 4) != 0) {
            schema.column("_kafka_rescue", DataTypes.STRING());
        }
        schema.column("_kafka_value", valueType(string));
        if ((mask & 1) != 0) {
            schema.column("_kafka_key", DataTypes.BYTES());
        }
        if ((mask & 8) != 0) {
            schema.column("_kafka_metadata", metadataType(true));
        }
        return table(descriptor(schema.build(), string).build());
    }

    private static DataType metadataType(boolean nullableTimestamp) {
        return DataTypes.ROW(
                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3).copy(nullableTimestamp)),
                DataTypes.FIELD("timestamp_type", DataTypes.INT().copy(false)),
                DataTypes.FIELD("partition", DataTypes.INT().copy(false)));
    }

    private static TableInfo metadataTable(
            boolean string, boolean nullableRow, boolean nullableTimestamp) {
        return table(
                descriptor(
                                Schema.newBuilder()
                                        .column(
                                                "_kafka_metadata",
                                                metadataType(nullableTimestamp).copy(nullableRow))
                                        .column("_kafka_value", valueType(string))
                                        .build(),
                                string)
                        .build());
    }

    private static DataType valueType(boolean string) {
        return string ? DataTypes.STRING() : DataTypes.BYTES();
    }

    private static TableDescriptor.Builder descriptor(Schema schema, boolean string) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(8)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(
                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, string ? "string" : "binary");
    }

    private static TableInfo table(TableDescriptor descriptor) {
        return TableInfo.of(TablePath.of("fluss", "topic"), 42L, 3, descriptor, null, 1L, 1L);
    }

    private static Map<String, byte[]> headers(InternalMap map) {
        Map<String, byte[]> result = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            assertThat(map.keyArray().isNullAt(i)).isFalse();
            String key = map.keyArray().getString(i).toString();
            assertThat(result).doesNotContainKey(key);
            result.put(key, map.valueArray().isNullAt(i) ? null : map.valueArray().getBytes(i));
        }
        return result;
    }

    private static void assertValue(InternalRow row, int position, boolean string, byte[] value) {
        if (string) {
            assertThat(row.getString(position).toString())
                    .isEqualTo(new String(value, StandardCharsets.UTF_8));
        } else {
            assertThat(row.getBytes(position)).isEqualTo(value);
        }
    }

    private static void assertMetadata(
            InternalRow row, int position, long timestamp, int timestampType, int partition) {
        assertThat(row.isNullAt(position)).isFalse();
        InternalRow metadata = row.getRow(position, 3);
        assertThat(metadata.getFieldCount()).isEqualTo(3);
        if (timestamp == Record.NO_TIMESTAMP) {
            assertThat(metadata.isNullAt(0)).isTrue();
        } else {
            assertThat(metadata.isNullAt(0)).isFalse();
            assertThat(metadata.getTimestampLtz(0, 3).getEpochMillisecond()).isEqualTo(timestamp);
        }
        assertThat(metadata.isNullAt(1)).isFalse();
        assertThat(metadata.getInt(1)).isEqualTo(timestampType);
        assertThat(metadata.isNullAt(2)).isFalse();
        assertThat(metadata.getInt(2)).isEqualTo(partition);
    }

    private static void read(
            BytesView encoded, TableInfo table, int count, Consumer<InternalRow> verify)
            throws Exception {
        ByteBuf buffer = encoded.getByteBuf();
        try {
            LogRecordBatch batch =
                    MemoryLogRecords.pointToByteBuffer(buffer.nioBuffer())
                            .batches()
                            .iterator()
                            .next();
            batch.ensureValid();
            assertThat(batch.schemaId()).isEqualTo((short) table.getSchemaId());
            assertThat(batch.getRecordCount()).isEqualTo(count);
            try (LogRecordReadContext context =
                            LogRecordReadContext.createArrowReadContext(
                                    table.getRowType(),
                                    table.getSchemaId(),
                                    new TestingSchemaGetter(
                                            new SchemaInfo(
                                                    table.getSchema(), table.getSchemaId())));
                    CloseableIterator<LogRecord> records = batch.records(context)) {
                for (int i = 0; i < count; i++) {
                    assertThat(records.hasNext()).isTrue();
                    verify.accept(records.next().getRow());
                }
                assertThat(records.hasNext()).isFalse();
            }
        } finally {
            buffer.release();
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
