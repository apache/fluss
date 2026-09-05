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
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.format.KafkaFormatFactoryRegistry;
import org.apache.fluss.kafka.schema.KafkaTopicSchema;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;

import java.util.ArrayList;
import java.util.List;

/** Transcodes Kafka records into native Arrow rows using a table-level Kafka format contract. */
@Internal
public final class ArrowKafkaRecordTranscoder implements KafkaRecordTranscoder {

    /** Default fixed-envelope key column created through Kafka CreateTopics. */
    public static final String KEY_COLUMN = "record_key";

    /** Default fixed-envelope value column created through Kafka CreateTopics. */
    public static final String VALUE_COLUMN = "payload";

    /** Default fixed-envelope timestamp column created through Kafka CreateTopics. */
    public static final String TIMESTAMP_COLUMN = "event_time";

    /** Default fixed-envelope headers column created through Kafka CreateTopics. */
    public static final String HEADERS_COLUMN = "headers";

    private final KafkaTopicSchemaResolver schemaResolver;
    private final KafkaFormatFactoryRegistry formatFactoryRegistry;
    private final FlussArrowRecordEncoder arrowRecordEncoder;

    /** Creates a transcoder with all built-in Kafka formats. */
    public ArrowKafkaRecordTranscoder() {
        this(
                new KafkaTopicSchemaResolver(),
                new KafkaFormatFactoryRegistry(),
                new FlussArrowRecordEncoder());
    }

    ArrowKafkaRecordTranscoder(
            KafkaTopicSchemaResolver schemaResolver,
            KafkaFormatFactoryRegistry formatFactoryRegistry,
            FlussArrowRecordEncoder arrowRecordEncoder) {
        this.schemaResolver = schemaResolver;
        this.formatFactoryRegistry = formatFactoryRegistry;
        this.arrowRecordEncoder = arrowRecordEncoder;
    }

    @Override
    public BytesView transcode(List<Record> records, TableInfo tableInfo) throws Exception {
        KafkaTopicSchema topicSchema = schemaResolver.resolve(tableInfo);
        KafkaFieldDecoder keyDecoder =
                topicSchema.keyFormat() == null
                        ? bytes -> new Object[0]
                        : formatFactoryRegistry.createDecoder(
                                topicSchema.keyFormat(), topicSchema.keyProjection(), null);
        KafkaFieldDecoder valueDecoder =
                formatFactoryRegistry.createDecoder(
                        topicSchema.valueFormat(),
                        topicSchema.valueProjection(),
                        topicSchema.valueRescueColumn());
        KafkaRowAssembler rowAssembler = new KafkaRowAssembler(topicSchema);
        List<GenericRow> rows = new ArrayList<>(records.size());
        for (Record record : records) {
            rows.add(
                    rowAssembler.assemble(
                            keyDecoder.decode(record.key()),
                            valueDecoder.decode(record.value()),
                            record.timestamp(),
                            record.headers()));
        }
        return arrowRecordEncoder.encode(rows, tableInfo);
    }
}
