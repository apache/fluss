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
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TimestampType;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Assembles decoded values and system columns using the shared fixed-column contract. */
@Internal
@ThreadSafe
public final class KafkaRowAssembler {

    private final KafkaTableSchema topicSchema;

    /** Creates an assembler for the resolved topic schema. */
    public KafkaRowAssembler(KafkaTableSchema topicSchema) {
        this.topicSchema = checkNotNull(topicSchema);
    }

    /**
     * Assembles one Fluss row from a record and its requested partition for immediate synchronous
     * encoding.
     *
     * <p>Key bytes are materialized from the command's read-only view for Arrow. The row borrows
     * header value arrays for immediate encoding; consumers must not modify them or retain the row
     * after conversion.
     */
    public GenericRow assemble(Object[] valueValues, Record record, int partitionId) {
        GenericRow row = new GenericRow(topicSchema.rowType().getFieldCount());
        setProjectedFields(row, topicSchema.valueProjection(), valueValues);
        if (topicSchema.keyPosition() >= 0) {
            int position = topicSchema.keyPosition();
            ByteBuffer key = record.key();
            row.setField(position, copyBytes(key));
        }
        if (topicSchema.headersPosition() >= 0) {
            int position = topicSchema.headersPosition();
            row.setField(position, toHeaders(record.headers()));
        }
        if (topicSchema.metadataPosition() >= 0) {
            int position = topicSchema.metadataPosition();
            row.setField(
                    position,
                    GenericRow.of(
                            record.hasTimestamp()
                                    ? TimestampLtz.fromEpochMillis(record.timestamp())
                                    : null,
                            timestampTypeId(record.timestampType()),
                            partitionId));
        }
        // A declared _kafka_rescue stays null for binary/string; omitted columns do not exist.
        return row;
    }

    static @Nullable byte[] copyBytes(@Nullable ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    private static int timestampTypeId(TimestampType type) {
        // Preserve batch provenance even when a record's timestamp is absent. The legacy record
        // constructor supplies UNKNOWN, so a present timestamp alone cannot imply CreateTime.
        switch (type) {
            case UNKNOWN:
                return -1;
            case CREATE_TIME:
                return 0;
            case LOG_APPEND_TIME:
                return 1;
            default:
                throw new KafkaRecordEncodingException("Unsupported Kafka timestamp type: " + type);
        }
    }

    private static void setProjectedFields(
            GenericRow row, KafkaFieldProjection projection, Object[] values) {
        if (values.length != projection.size()) {
            throw new IllegalArgumentException(
                    "Kafka decoder returned "
                            + values.length
                            + " fields for a projection of "
                            + projection.size()
                            + ".");
        }
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            row.setField(projection.positionAt(i), value);
        }
    }

    private static GenericMap toHeaders(List<RecordHeader> headers) {
        Map<BinaryString, byte[]> values = new LinkedHashMap<>();
        // Walk backwards to retain only the last value for each header name.
        for (int i = headers.size() - 1; i >= 0; i--) {
            RecordHeader header = headers.get(i);
            BinaryString name = BinaryString.fromString(header.name());
            if (!values.containsKey(name)) {
                values.put(name, header.value());
            }
        }
        return new GenericMap(values);
    }
}
