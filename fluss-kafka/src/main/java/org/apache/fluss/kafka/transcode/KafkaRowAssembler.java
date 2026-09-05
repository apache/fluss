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
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.schema.KafkaFieldProjection;
import org.apache.fluss.kafka.schema.KafkaTopicSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;

import java.util.List;

/** Assembles decoded Kafka key, value, and metadata into a physical Fluss row. */
@Internal
public final class KafkaRowAssembler {

    private final KafkaTopicSchema topicSchema;

    /** Creates an assembler for the resolved topic schema. */
    public KafkaRowAssembler(KafkaTopicSchema topicSchema) {
        this.topicSchema = topicSchema;
    }

    /** Assembles one Fluss row. */
    public GenericRow assemble(
            Object[] keyValues, Object[] valueValues, long timestamp, List<RecordHeader> headers) {
        GenericRow row = new GenericRow(topicSchema.rowType().getFieldCount());
        setProjectedFields(row, topicSchema.keyProjection(), keyValues);
        setProjectedFields(row, topicSchema.valueProjection(), valueValues);
        if (topicSchema.timestampPosition() >= 0) {
            row.setField(topicSchema.timestampPosition(), TimestampLtz.fromEpochMillis(timestamp));
        }
        if (topicSchema.headersPosition() >= 0) {
            row.setField(topicSchema.headersPosition(), toHeaders(headers));
        }
        return row;
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
            if (value == null && !projection.dataTypeAt(i).isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Kafka record cannot populate NOT NULL field '"
                                + projection.nameAt(i)
                                + "' with null.");
            }
            row.setField(projection.positionAt(i), value);
        }
    }

    private static GenericArray toHeaders(List<RecordHeader> headers) {
        Object[] rows = new Object[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            RecordHeader header = headers.get(i);
            rows[i] = GenericRow.of(BinaryString.fromString(header.name()), header.value());
        }
        return new GenericArray(rows);
    }
}
