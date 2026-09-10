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

import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
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
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Verifies mapped Arrow records after transcoding resources have closed. */
class ArrowKafkaRecordTranscoderTest {
    private final ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder();

    @Test
    void testRawAndStringMappingsUsePhysicalColumnPositions() throws Exception {
        for (boolean string : new boolean[] {false, true}) {
            TableInfo table = envelope(string);
            Record record =
                    new Record(
                            123L,
                            bytes("键"),
                            bytes("message"),
                            Arrays.asList(
                                    new RecordHeader("duplicate", bytes("first")),
                                    new RecordHeader("duplicate", null)));
            BytesView encoded = transcoder.transcode(Arrays.asList(record, record), table);
            read(
                    encoded,
                    table,
                    2,
                    row -> {
                        if (string) {
                            assertThat(row.getString(0).toString()).isEqualTo("message");
                            assertThat(row.getString(3).toString()).isEqualTo("键");
                        } else {
                            assertThat(row.getBytes(0)).isEqualTo(bytes("message"));
                            assertThat(row.getBytes(3)).isEqualTo(bytes("键"));
                        }
                        assertThat(row.getTimestampLtz(1, 3).getEpochMillisecond()).isEqualTo(123L);
                        InternalArray headers = row.getArray(2);
                        assertThat(headers.size()).isEqualTo(2);
                        assertThat(headers.getRow(0, 2).getString(0).toString())
                                .isEqualTo("duplicate");
                        assertThat(headers.getRow(0, 2).getBytes(1)).isEqualTo(bytes("first"));
                        assertThat(headers.getRow(1, 2).isNullAt(1)).isTrue();
                    });
        }
    }

    @Test
    void testNullsAndEmptyBytesRemainDistinct() throws Exception {
        TableInfo table = envelope(false);
        read(
                transcoder.transcode(
                        Collections.singletonList(
                                new Record(1L, null, null, Collections.emptyList())),
                        table),
                table,
                1,
                row -> {
                    assertThat(row.isNullAt(0)).isTrue();
                    assertThat(row.isNullAt(3)).isTrue();
                    assertThat(row.getArray(2).size()).isZero();
                });
        read(
                transcoder.transcode(
                        Collections.singletonList(
                                new Record(1L, new byte[0], new byte[0], Collections.emptyList())),
                        table),
                table,
                1,
                row -> {
                    assertThat(row.getBytes(0)).isEmpty();
                    assertThat(row.getBytes(3)).isEmpty();
                });
    }

    @Test
    void testUnmappedKeyIsIgnoredAndMixedFormatsWork() throws Exception {
        TableInfo valueOnly = valueTable(false);
        read(
                transcoder.transcode(
                        Collections.singletonList(
                                new Record(
                                        1L,
                                        new byte[] {(byte) 0xff},
                                        bytes("value"),
                                        Collections.emptyList())),
                        valueOnly),
                valueOnly,
                1,
                row -> assertThat(row.getString(0).toString()).isEqualTo("value"));
        Schema schema =
                Schema.newBuilder()
                        .column("body", DataTypes.BYTES())
                        .column("id", DataTypes.STRING())
                        .build();
        TableInfo mixed =
                table(
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(1)
                                .logFormat(LogFormat.ARROW)
                                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw")
                                .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                                .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "id")
                                .customProperty(
                                        KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                                .build());
        read(
                transcoder.transcode(
                        Collections.singletonList(
                                new Record(
                                        1L,
                                        bytes("key"),
                                        new byte[] {(byte) 0xff},
                                        Collections.emptyList())),
                        mixed),
                mixed,
                1,
                row -> {
                    assertThat(row.getString(1).toString()).isEqualTo("key");
                    assertThat(row.getBytes(0)).containsExactly((byte) 0xff);
                });
    }

    @Test
    void testRejectsMalformedUtf8AndNotNullViolationsThenRemainsUsable() throws Exception {
        TableInfo table = valueTable(false);
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        Collections.singletonList(
                                                new Record(
                                                        1L,
                                                        null,
                                                        new byte[] {(byte) 0xc3, 0x28},
                                                        Collections.emptyList())),
                                        table))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("UTF-8");
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        Collections.singletonList(
                                                new Record(
                                                        1L, null, null, Collections.emptyList())),
                                        valueTable(true)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("NOT NULL");
        read(
                transcoder.transcode(
                        Collections.singletonList(
                                new Record(1L, null, bytes("ok"), Collections.emptyList())),
                        table),
                table,
                1,
                row -> assertThat(row.getString(0).toString()).isEqualTo("ok"));
    }

    @Test
    void testRejectsInvalidMappingAndEmptyPartition() {
        TableInfo invalid =
                table(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("body", DataTypes.STRING())
                                                .build())
                                .distributedBy(1)
                                .build());
        assertThatThrownBy(
                        () ->
                                transcoder.transcode(
                                        Collections.singletonList(
                                                new Record(
                                                        1L,
                                                        null,
                                                        bytes("value"),
                                                        Collections.emptyList())),
                                        invalid))
                .isInstanceOf(KafkaTopicSchemaException.class);
        assertThatThrownBy(() -> transcoder.transcode(Collections.emptyList(), valueTable(false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
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

    private static TableInfo valueTable(boolean notNull) {
        return table(
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("body", DataTypes.STRING().copy(!notNull))
                                        .build())
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string")
                        .build());
    }

    private static TableInfo envelope(boolean string) {
        Schema schema =
                Schema.newBuilder()
                        .column("body", string ? DataTypes.STRING() : DataTypes.BYTES())
                        .column("time", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                        .column(
                                "attrs",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("value", DataTypes.BYTES()))))
                        .column("id", string ? DataTypes.STRING() : DataTypes.BYTES())
                        .build();
        return table(
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .customProperty(
                                KafkaDataFormat.VALUE_FORMAT_CONFIG, string ? "string" : "raw")
                        .customProperty(
                                KafkaDataFormat.KEY_FORMAT_CONFIG, string ? "string" : "raw")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "id")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "time")
                        .customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "attrs")
                        .build());
    }

    private static TableInfo table(TableDescriptor descriptor) {
        return TableInfo.of(TablePath.of("kafka", "topic"), 42L, 3, descriptor, null, 1L, 1L);
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
