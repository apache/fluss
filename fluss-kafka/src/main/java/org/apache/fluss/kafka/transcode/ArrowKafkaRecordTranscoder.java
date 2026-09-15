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
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.schema.KafkaTopicSchema;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Converts raw/string Kafka records using the DDL mapping into owned Fluss Arrow log bytes. */
@Internal
public final class ArrowKafkaRecordTranscoder implements KafkaRecordTranscoder {
    private final KafkaTopicSchemaResolver schemaResolver = new KafkaTopicSchemaResolver();
    private final FlussArrowRecordEncoder arrowRecordEncoder = new FlussArrowRecordEncoder();

    @Override
    public BytesView transcode(List<Record> records, TableInfo tableInfo) throws Exception {
        checkArgument(!records.isEmpty(), "Cannot transcode an empty Kafka partition.");
        KafkaTopicSchema schema = schemaResolver.resolve(tableInfo.toTableDescriptor());
        KafkaRowAssembler assembler = new KafkaRowAssembler(schema);
        List<GenericRow> rows = new ArrayList<>(records.size());
        for (Record record : records) {
            Object[] key =
                    schema.keyFormat() == null
                            ? new Object[0]
                            : new Object[] {decode(schema.keyFormat(), record.key())};
            rows.add(
                    assembler.assemble(
                            key,
                            new Object[] {decode(schema.valueFormat(), record.value())},
                            record.timestamp(),
                            record.headers()));
        }
        return arrowRecordEncoder.encode(rows, tableInfo);
    }

    private static @Nullable Object decode(KafkaDataFormat format, @Nullable byte[] bytes) {
        if (bytes == null || format == KafkaDataFormat.RAW) {
            return bytes;
        }
        if (format != KafkaDataFormat.STRING) {
            throw new KafkaRecordEncodingException("Unsupported Kafka data format: " + format);
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
            throw new KafkaRecordEncodingException("Kafka string field is not valid UTF-8.", e);
        }
    }
}
