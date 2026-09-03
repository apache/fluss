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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Transcodes Kafka records into the fixed-schema Arrow log table used for Kafka topics. */
@Internal
public final class ArrowKafkaRecordTranscoder implements KafkaRecordTranscoder {

    /** Column containing the nullable Kafka record key. */
    public static final String KEY_COLUMN = "record_key";

    /** Column containing the nullable Kafka record value. */
    public static final String VALUE_COLUMN = "payload";

    /** Column containing the Kafka record timestamp. */
    public static final String TIMESTAMP_COLUMN = "event_time";

    /** Column containing the Kafka record headers. */
    public static final String HEADERS_COLUMN = "headers";

    private static final String[] COLUMN_NAMES = {
        KEY_COLUMN, VALUE_COLUMN, TIMESTAMP_COLUMN, HEADERS_COLUMN
    };
    private static final int INITIAL_PAGE_SIZE = 4096;

    @Override
    public BytesView transcode(List<Record> records, TableInfo tableInfo) throws Exception {
        validateTable(tableInfo);
        KafkaDataFormat keyFormat = dataFormat(tableInfo, KafkaDataFormat.KEY_FORMAT_CONFIG);
        KafkaDataFormat valueFormat = dataFormat(tableInfo, KafkaDataFormat.VALUE_FORMAT_CONFIG);
        RowType rowType = tableInfo.getRowType();
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    provider.getOrCreateWriter(
                            tableInfo.getTableId(),
                            tableInfo.getSchemaId(),
                            Integer.MAX_VALUE,
                            rowType,
                            tableInfo.getTableConfig().getArrowCompressionInfo());
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            tableInfo.getSchemaId(),
                            writer,
                            new UnmanagedPagedOutputView(INITIAL_PAGE_SIZE),
                            true,
                            null);
            for (Record record : records) {
                builder.append(
                        ChangeType.APPEND_ONLY,
                        GenericRow.of(
                                transcodeBytes(record.key(), keyFormat, KEY_COLUMN),
                                transcodeBytes(record.value(), valueFormat, VALUE_COLUMN),
                                TimestampLtz.fromEpochMillis(record.timestamp()),
                                toHeaders(record.headers())));
            }
            return builder.build();
        }
    }

    private static void validateTable(TableInfo tableInfo) {
        checkArgument(!tableInfo.hasPrimaryKey(), "Kafka topic table must be a log table.");
        checkArgument(!tableInfo.isPartitioned(), "Partitioned Fluss tables are not supported.");
        checkArgument(
                tableInfo.getTableConfig().getLogFormat() == LogFormat.ARROW,
                "Kafka topic table must use the Arrow log format.");
        RowType rowType = tableInfo.getRowType();
        KafkaDataFormat keyFormat = dataFormat(tableInfo, KafkaDataFormat.KEY_FORMAT_CONFIG);
        KafkaDataFormat valueFormat = dataFormat(tableInfo, KafkaDataFormat.VALUE_FORMAT_CONFIG);
        checkArgument(
                rowType.getFieldNames().equals(Arrays.asList(COLUMN_NAMES)),
                "Kafka topic table columns must be %s.",
                Arrays.toString(COLUMN_NAMES));
        checkDataType(rowType, 0, KEY_COLUMN, keyFormat);
        checkDataType(rowType, 1, VALUE_COLUMN, valueFormat);
        checkArgument(
                rowType.getTypeAt(2) instanceof LocalZonedTimestampType
                        && !rowType.getTypeAt(2).isNullable()
                        && ((LocalZonedTimestampType) rowType.getTypeAt(2)).getPrecision() == 3,
                "Kafka event_time column must be TIMESTAMP_LTZ(3) NOT NULL.");
        checkHeadersType(rowType);
    }

    private static GenericArray toHeaders(List<RecordHeader> headers) {
        Object[] rows = new Object[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            RecordHeader header = headers.get(i);
            rows[i] = GenericRow.of(BinaryString.fromString(header.name()), header.value());
        }
        return new GenericArray(rows);
    }

    private static KafkaDataFormat dataFormat(TableInfo tableInfo, String configKey) {
        String value = tableInfo.getCustomProperties().toMap().get(configKey);
        return value == null ? KafkaDataFormat.RAW : KafkaDataFormat.parse(value);
    }

    private static void checkDataType(
            RowType rowType, int position, String columnName, KafkaDataFormat format) {
        boolean validType =
                format == KafkaDataFormat.RAW
                        ? rowType.getTypeAt(position) instanceof BytesType
                        : rowType.getTypeAt(position) instanceof StringType;
        checkArgument(
                validType && rowType.getTypeAt(position).isNullable(),
                "Kafka %s column must be nullable %s for format %s.",
                columnName,
                format == KafkaDataFormat.RAW ? "BYTES" : "STRING",
                format.value());
    }

    private static @Nullable Object transcodeBytes(
            @Nullable byte[] bytes, KafkaDataFormat format, String columnName) {
        if (bytes == null || format == KafkaDataFormat.RAW) {
            return bytes;
        }
        try {
            return BinaryString.fromString(
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(bytes))
                            .toString());
        } catch (CharacterCodingException e) {
            throw new KafkaRecordEncodingException(
                    "Kafka " + columnName + " is not valid UTF-8 for string format.", e);
        }
    }

    private static void checkHeadersType(RowType rowType) {
        checkArgument(
                rowType.getTypeAt(3) instanceof ArrayType && rowType.getTypeAt(3).isNullable(),
                "Kafka headers column must be nullable ARRAY<ROW<name STRING, value BYTES>>.");
        ArrayType headersType = (ArrayType) rowType.getTypeAt(3);
        checkArgument(
                headersType.getElementType() instanceof RowType,
                "Kafka headers elements must be ROW<name STRING, value BYTES>.");
        RowType headerType = (RowType) headersType.getElementType();
        checkArgument(
                headerType.getFieldNames().equals(Arrays.asList("name", "value"))
                        && headerType.getTypeAt(0) instanceof StringType
                        && !headerType.getTypeAt(0).isNullable()
                        && headerType.getTypeAt(1) instanceof BytesType
                        && headerType.getTypeAt(1).isNullable(),
                "Kafka headers elements must be ROW<name STRING NOT NULL, value BYTES>.");
    }
}
